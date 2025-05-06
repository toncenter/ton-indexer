from __future__ import annotations

from pytoniq_core import Address, Cell, ExternalAddress, Slice, InternalMsgInfo


# ------------------------- Supply -------------------------

class EvaaSupplyMaster:
    # owner -> master
    # schema.tlb#L184
    # supply_master#00000001 query_id:uint64 include_user_code:BoolExtended supply_amount:Amount recipient_address:MsgAddressInt
    #    forward_ton_amount:Amount custom_response_payload:^Cell = InternalMsgBody; // * -> Master
    opcode = 0x1  # op::supply_master
    
    query_id: int
    include_user_code: bool
    supply_amount: int
    recipient_address: Address
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.include_user_code = slice.load_int(2) != 0
        self.supply_amount = slice.load_uint(64)  # amount type in schema
        self.recipient_address = slice.load_address()
        # skip rest - not needed for indexing
class EvaaSupplyJettonForwardMessage:
    # owner -> master
    # schema.tlb#L184
    # supply_master#00000001 query_id:uint64 include_user_code:BoolExtended supply_amount:Amount recipient_address:MsgAddressInt
    #    forward_ton_amount:Amount custom_response_payload:^Cell = InternalMsgBody; // * -> Master
    opcode = 0x1  # op::supply_master

    include_user_code: bool
    forward_amount: int | None
    recipient_address: Address
    custom_response_payload: Cell | None

    def __init__(self, slice: Slice, skip_query_id: bool = False):
        opcode = slice.load_uint(32)
        assert opcode == self.opcode, f"Invalid opcode: {opcode}"

        self.include_user_code = slice.load_int(2) != 0
        self.recipient_address = slice.load_address()
        if slice.remaining_bits > 0:
            self.forward_amount = slice.load_uint(64)  # amount type in schema
            self.custom_response_payload = slice.load_ref()
        else:
            self.forward_amount = None
            self.custom_response_payload = None

class EvaaSupplyUser:
    # master -> user
    # schema.tlb#L186
    # supply_user#00000011 query_id:uint64 asset_id:AssetId
    #    supply_amount_current:Amount s_rate:SbRate b_rate:SbRate
    #    dust:uint64 max_token_amount:uint64 total_supply:Principal total_borrow:Principal
    #    tracking_supply_index:TrackingIndex tracking_borrow_index:TrackingIndex
    #    forward_ton_amount:Amount custom_response_payload:^Cell = UserCommand;
    opcode = 0x11  # op::supply_user
    response_opcode = 0xEF8C8068
    
    query_id: int
    asset_id: int
    supply_amount_current: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.asset_id = slice.load_uint(256)  # assetId in schema
        self.supply_amount_current = slice.load_uint(64)  # amount in schema
        # skip rest


class EvaaSupplySuccess:
    # user -> master
    # schema.tlb#L190
    # supply_success#0000011a query_id:uint64 owner_address:MsgAddressInt
    #    asset_id:AssetId amount_supplied:Amount user_new_principal:Principal
    #    repay_amount_principal:Principal supply_amount_principal:Principal
    #    custom_response_payload:^Cell = InternalMsgBody;
    opcode = 0x11a  # op::supply_success
    
    query_id: int
    owner_address: Address
    asset_id: int
    amount_supplied: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.owner_address = slice.load_address()
        self.asset_id = slice.load_uint(256)
        self.amount_supplied = slice.load_uint(64)
        # skip rest


class EvaaSupplyFail:
    # user -> master
    # schema.tlb#L194
    # supply_fail#0000011f query_id:uint64 owner_address:MsgAddressInt asset_id:AssetId amount:Amount
    #    forward_ton_amount:Amount custom_response_payload:^Cell = InternalMsgBody;
    opcode = 0x11f  # op::supply_fail
    
    query_id: int
    owner_address: Address
    asset_id: int
    amount: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.owner_address = slice.load_address()
        self.asset_id = slice.load_uint(256)
        self.amount = slice.load_uint(64)
        # skip rest


# ------------------------- Withdraw -------------------------

class EvaaWithdrawMaster:
    # owner -> master
    # schema.tlb#L200
    # withdraw_master#00000002 query_id:uint64 asset_id:AssetId amount:Amount recipient_addr:MsgAddressInt
    #    include_user_code:BoolExtended forward_ton_amount:Amount custom_response_payload:^Cell
    #    prices_with_signature_packed:^PricesPacked = InternalMsgBody;
    opcode = 0x2  # op::withdraw_master
    
    query_id: int
    asset_id: int
    amount: int
    recipient_address: Address
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.asset_id = slice.load_uint(256)
        self.amount = slice.load_uint(64)
        self.recipient_address = slice.load_address()
        # skip rest


class EvaaWithdrawUser:
    # master -> user
    # schema.tlb#L202
    # withdraw_user#00000021 query_id:uint64 asset_id:AssetId
    #    withdraw_amount_current:Amount s_rate:SbRate b_rate:SbRate recipient_address:MsgAddressInt
    #    ^[ asset_config_collection:AssetConfigCollection asset_dynamics_collection:AssetDynamicsCollection
    #       prices_packed:^PricesPacked forward_ton_amount:Amount custom_response_payload:^Cell ] = UserCommand;
    opcode = 0x21  # op::withdraw_user
    response_opcode = 0xFA6B3DED
    
    query_id: int
    asset_id: int
    withdraw_amount_current: int
    recipient_address: Address
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.asset_id = slice.load_uint(256)
        self.withdraw_amount_current = slice.load_uint(64)
        slice.load_uint(64)  # s_rate
        slice.load_uint(64)  # b_rate
        self.recipient_address = slice.load_address()
        # skip reference fields


class EvaaWithdrawCollateralized:
    # user -> master
    # schema.tlb#L206
    # withdraw_collateralized#00000211 query_id:uint64 owner_address:MsgAddressInt
    #    asset_id:AssetId withdraw_amount_current:Amount
    #    user_new_principal:Principal borrow_amount_principal:Principal reclaim_amount_principal:Principal
    #    ^[ recipient_address:MsgAddressInt forward_ton_amount:Amount custom_response_payload:^Cell ] = InternalMsgBody;
    opcode = 0x211  # op::withdraw_collateralized
    
    query_id: int
    owner_address: Address
    asset_id: int
    withdraw_amount_current: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.owner_address = slice.load_address()
        self.asset_id = slice.load_uint(256)
        self.withdraw_amount_current = slice.load_uint(64)
        # skip rest


class EvaaWithdrawSuccess:
    # master -> user
    # schema.tlb#L210
    # withdraw_success#0000211a query_id:uint64 asset_id:AssetId principal_amount:Principal
    #    tracking_supply_index:TrackingIndex tracking_borrow_index:TrackingIndex = UserCommand;
    opcode = 0x211a  # op::withdraw_success
    
    query_id: int
    asset_id: int
    principal_amount: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.asset_id = slice.load_uint(256)
        self.principal_amount = slice.load_int(64)
        # skip rest


class EvaaWithdrawFail:
    # master -> user
    # schema.tlb#L211
    # withdraw_fail#0000211f query_id:uint64 asset_id:AssetId principal_amount:Principal = UserCommand;
    opcode = 0x211f  # op::withdraw_fail
    
    query_id: int
    asset_id: int
    principal_amount: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.asset_id = slice.load_uint(256)
        self.principal_amount = slice.load_int(64)


class EvaaWithdrawFailExcess:
    # user -> master -> user contract (error!) ->(one of this opcodes) user
    opcode: int
    reason: str
    opcodes = [0x21e6, 0x21e7, 0x21e8, 0x21ec]

    def __init__(self, slice: Slice):
        op = slice.load_uint(32)
        self.opcode = op
        if op == 0x21e6:
            self.reason = "withdraw_locked_excess"
        elif op == 0x21e7:
            self.reason = "withdraw_not_collateralized_excess"
        elif op == 0x21e8:
            self.reason = "withdraw_missing_prices_excess"
        elif op == 0x21ec:
            self.reason = "withdraw_execution_crashed"
        else:
            raise ValueError(f"Unknown withdraw fail opcode: {op}")

class EvaaWithdrawNoFundsExcess:
    # it's not in prev list because it happens 
    # when error occurs later - on master step:
    # user -> master -> user contract -> master (error!) -> user contract ->(this opcode) user
    opcode = 0x211fe8
    reason = "withdraw_no_funds_excess"

# ------------------------- Liquidate -------------------------

class EvaaLiquidateMaster:
    # liquidator -> master
    # schema.tlb#L236
    # liquidate_master#00000003 query_id:uint64 borrower_address:MsgAddressInt liquidator_address:MsgAddressInt
    #    collateral_asset_id:AssetId min_collateral_amount:Amount include_user_code:BoolExtended
    #    liquidate_incoming_amount:Amount ^[ forward_ton_amount:Amount custom_response_payload:^Cell ]
    #    prices_with_signature_packed:^PricesPacked = InternalMsgBody;
    opcode = 0x3  # op::liquidate_master
    
    query_id: int
    borrower_address: Address
    liquidator_address: Address
    collateral_asset_id: int
    liquidate_incoming_amount: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.borrower_address = slice.load_address()
        self.liquidator_address = slice.load_address()
        self.collateral_asset_id = slice.load_uint(256)
        slice.load_uint(64)  # min_collateral_amount
        slice.load_int(2)  # include_user_code
        self.liquidate_incoming_amount = slice.load_uint(64)
        # skip rest


class EvaaLiquidateUser:
    # master -> user
    # schema.tlb#L239
    # liquidate_user#00000031 query_id:uint64
    #    asset_config_collection:AssetConfigCollection asset_dynamics_collection:AssetDynamicsCollection
    #    ^[ prices_packed:^PricesPacked collateral_asset_id:AssetId min_collateral_amount:Amount
    #       liquidator_address:MsgAddressInt transferred_asset_id:AssetId transfered_amount:Amount
    #       forward_ton_amount:Amount custom_response_payload:^Cell ] = UserCommand;
    opcode = 0x31  # op::liquidate_user
    response_opcode = 0xEE8F0607

    query_id: int
    liquidator_address: Address
    collateral_asset_id: int
    transferred_asset_id: int
    transferred_amount: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        slice.load_dict()  # asset_config_collection
        slice.load_dict()  # asset_dynamics_collection
        
        ref_data = slice.load_ref().begin_parse()
        ref_data.load_ref()  # prices_packed
        self.collateral_asset_id = ref_data.load_uint(256)
        ref_data.load_uint(64)  # min_collateral_amount
        self.liquidator_address = ref_data.load_address()
        self.transferred_asset_id = ref_data.load_uint(256)
        self.transferred_amount = ref_data.load_uint(64)
        # skip rest


class EvaaLiquidateSatisfied:
    # user -> master
    # schema.tlb#L248
    # liquidate_satisfied#00000311 query_id:uint64 owner_address:MsgAddressInt
    #    liquidator_address:MsgAddressInt transferred_asset_id:AssetId
    #    ^[ delta_loan_principal:Principal liquidatable_amount:Amount protocol_gift:Amount
    #       new_user_loan_principal:Principal collateral_asset_id:AssetId delta_collateral_principal:Principal
    #       collateral_reward:Amount min_collateral_amount:Amount new_user_collateral_principal:Principal
    #       forward_ton_amount:Amount custom_response_payload:^Cell ]= InternalMsgBody;
    opcode = 0x311  # op::liquidate_satisfied
    
    query_id: int
    owner_address: Address
    liquidator_address: Address
    transferred_asset_id: int
    delta_loan_principal: int
    liquidatable_amount: int
    protocol_gift: int
    new_user_loan_principal: int
    collateral_asset_id: int
    delta_collateral_principal: int
    collateral_reward: int
    min_collateral_amount: int
    new_user_collateral_principal: int
    forward_ton_amount: int
    custom_response_payload: Cell

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.owner_address = slice.load_address()
        self.liquidator_address = slice.load_address()
        self.transferred_asset_id = slice.load_uint(256)
        
        ref_data = slice.load_ref().begin_parse()
        self.delta_loan_principal = ref_data.load_int(64)
        self.liquidatable_amount = ref_data.load_uint(64)
        self.protocol_gift = ref_data.load_uint(64)
        self.new_user_loan_principal = ref_data.load_int(64)
        self.collateral_asset_id = ref_data.load_uint(256)
        self.delta_collateral_principal = ref_data.load_int(64)
        self.collateral_reward = ref_data.load_uint(64)
        if ref_data.remaining_bits > 0:
            self.min_collateral_amount = ref_data.load_uint(64)
            self.new_user_collateral_principal = ref_data.load_int(64)
            self.forward_ton_amount = ref_data.load_uint(64)
            self.custom_response_payload = ref_data.load_ref()
        else:
            self.min_collateral_amount = None
            self.new_user_collateral_principal = None
            self.forward_ton_amount = None
            self.custom_response_payload = None


class EvaaLiquidateUnsatisfied:
    # user -> master
    # schema.tlb#L243
    # liquidate_unsatisfied#0000031f query_id:uint64 owner_address:MsgAddressInt
    #    liquidator_address:MsgAddressInt transferred_asset_id:AssetId
    #    ^[ transferred_amount:Amount collateral_asset_id:AssetId min_collateral_amount:Amount
    #       forward_ton_amount:Amount custom_response_payload:^Cell error:LiquidationError ]= InternalMsgBody;
    opcode = 0x31f  # op::liquidate_unsatisfied

    query_id: int
    owner_address: Address
    liquidator_address: Address
    transferred_asset_id: int
    transferred_amount: int
    collateral_asset_id: int
    min_collateral_amount: int
    forward_ton_amount: int
    custom_response_payload: Cell
    error_slice: Slice

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.owner_address = slice.load_address()
        self.liquidator_address = slice.load_address()
        self.transferred_asset_id = slice.load_uint(256)
        
        ref_data = slice.load_ref().begin_parse()
        self.transferred_amount = ref_data.load_uint(64)
        self.collateral_asset_id = ref_data.load_uint(256)
        self.min_collateral_amount = ref_data.load_uint(64)
        self.forward_ton_amount = ref_data.load_uint(64)
        self.custom_response_payload = ref_data.load_ref()
        self.error_slice = ref_data  # оставшаяся часть slice содержит данные об ошибке
        
    def get_error(self) -> EvaaLiquidationError:
        """Возвращает объект ошибки ликвидации из данных сообщения"""
        return EvaaLiquidationError(self.error_slice)


class EvaaLiquidateSuccessReport:
    # master -> liquidator
    # schema.tlb
    # liquidate_success_report#0000311d query_id:uint64 
    #    transferred_asset_id:AssetId transferred_amount:Amount
    #    collateral_asset_id:AssetId collateral_reward:Amount
    #    custom_response_payload:^Cell = InternalMsgBody;
    opcode = 0x311d  # op::liquidate_success_report
    
    query_id: int
    transferred_asset_id: int
    transferred_amount: int
    collateral_asset_id: int
    collateral_reward: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.transferred_asset_id = slice.load_uint(256)
        self.transferred_amount = slice.load_uint(64)
        self.collateral_asset_id = slice.load_uint(256)
        self.collateral_reward = slice.load_uint(64)
        # skip custom_response_payload reference


class EvaaLiquidateSuccess:
    # master -> user
    # schema.tlb
    # liquidate_success#0000311a query_id:uint64 
    #    transferred_asset_id:AssetId 
    #    delta_loan_principal:Principal 
    #    loan_tracking_supply_index:TrackingIndex loan_tracking_borrow_index:TrackingIndex
    #    collateral_asset_id:AssetId 
    #    delta_collateral_principal:Principal
    #    collateral_tracking_supply_index:TrackingIndex collateral_tracking_borrow_index:TrackingIndex = UserCommand;
    opcode = 0x311a  # op::liquidate_success
    
    query_id: int
    transferred_asset_id: int
    delta_loan_principal: int
    collateral_asset_id: int
    delta_collateral_principal: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.transferred_asset_id = slice.load_uint(256)
        self.delta_loan_principal = slice.load_int(64)
        slice.load_uint(64)  # loan_tracking_supply_index
        slice.load_uint(64)  # loan_tracking_borrow_index
        self.collateral_asset_id = slice.load_uint(256)
        self.delta_collateral_principal = slice.load_int(64)
        # skip tracking indices


class EvaaLiquidateFail:
    # master -> user
    # schema.tlb
    # liquidate_fail#0000311f query_id:uint64 
    #    transferred_asset_id:AssetId delta_loan_principal:Principal
    #    collateral_asset_id:AssetId delta_collateral_principal:Principal = UserCommand;
    opcode = 0x311f  # op::liquidate_fail
    
    query_id: int
    transferred_asset_id: int
    delta_loan_principal: int
    collateral_asset_id: int
    delta_collateral_principal: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.transferred_asset_id = slice.load_uint(256)
        self.delta_loan_principal = slice.load_int(64)
        self.collateral_asset_id = slice.load_uint(256)
        self.delta_collateral_principal = slice.load_int(64)


class EvaaLiquidationError:
    """Класс для обработки ошибок ликвидации из сообщений liquidate_unsatisfied.
    
    Все возможные ошибки определены в liquidate-message.fc:
    - master_liquidating_too_much (0xE001)
    - user_withdraw_in_progress (0xE002)
    - not_liquidatable (0xE003)
    - liqudation_execution_crashed (0xE004)
    - min_collateral_not_satisfied (0xE005)
    - user_not_enough_collateral (0xE006)
    - user_liquidating_too_much (0xE007)
    - master_not_enough_liquidity (0xE008)
    - liquidation_prices_missing (0xE009)
    """
    
    reason: str
    error_opcode: int
    
    # Карта опкодов ошибок из liquidate-message.fc
    ERROR_CODES = {
        0xE001: "master_liquidating_too_much",
        0xE002: "user_withdraw_in_progress",
        0xE003: "not_liquidatable",
        0xE004: "execution_crashed",
        0xE005: "min_collateral_not_satisfied",
        0xE006: "user_not_enough_collateral",
        0xE007: "user_liquidating_too_much",
        0xE008: "master_not_enough_liquidity",
        0xE009: "liquidation_prices_missing"
    }

    def __init__(self, slice: Slice):
        """Инициализирует объект ошибки ликвидации из slice.
        
        Args:
            slice: Slice с данными об ошибке (обычно из parse_liquidate_unsatisfied_message)
        """
        try:
            self.error_opcode = slice.load_uint(32)
            self.reason = self.ERROR_CODES.get(self.error_opcode, "unknown")
        except Exception:
            self.error_opcode = 0
            self.reason = "parse_error"
    
    def __str__(self):
        return f"LiquidationError: {self.reason} (0x{self.error_opcode:X})"


# ------------------------- Other -------------------------

class EvaaRevertCall:
    # contract -> sender
    # schema.tlb#L180
    # revert_call#0000000f query_id:uint64 owner_address:MsgAddressInt revert_body:^Cell = InternalMsgBody;
    opcode = 0xF  # op::revert_call
    
    query_id: int
    owner_address: Address

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        self.owner_address = slice.load_address()
        # skip revert_body - not needed for indexing


class EvaaIdleUser:
    # master -> user
    # schema.tlb#L267
    # idle_user#00000081 query_id:uint64 tokens_keys:(Maybe ^Cell) originator_address:MsgAddressInt = UserCommand;
    opcode = 0x81  # op::idle_user
    
    query_id: int
    originator_address: Address
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # op
        self.query_id = slice.load_uint(64)
        maybe_bit = slice.load_uint(1)
        if maybe_bit:
            slice.load_ref()  # tokens_keys
        self.originator_address = slice.load_address()

