from __future__ import annotations

from pytoniq_core import Slice, Address, Cell

from indexer.events.blocks.utils import Asset


class StonfiSwapMessage:
    opcode = 0x25938561

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.from_user_address = body.load_address()
        self.token_wallet = body.load_address()
        self.amount = body.load_coins()
        self.min_out = body.load_coins()
        self.has_ref = body.load_bit()
        ref = body.load_ref().to_slice()
        self.from_real_user = ref.load_address()
        self.ref_address = None
        if self.has_ref:
            self.ref_address = ref.load_address()


class StonfiPaymentRequest:
    opcode = 0xf93bb43f

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.owner = body.load_address()
        self.exit_code = body.load_uint(32)
        ref = body.load_ref().to_slice()
        self.amount0_out = ref.load_coins()
        self.token0_out = ref.load_address()
        self.amount1_out = ref.load_coins()
        self.token1_out = ref.load_address()


def load_asset(slice: Slice) -> Asset:
    kind = slice.load_uint(4)
    if kind == 0:
        return Asset(True)
    else:
        wc = slice.load_uint(8)
        account_id = slice.load_bytes(32)
        return Asset(False, Address((wc, account_id)))

class PTonTransfer:
    # ton_transfer query_id:uint64 ton_amount:Coins refund_address:MsgAddress forward_payload:(Either Cell ^Cell) = InternalMsgBody;
    opcode = 0x01f3835d
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.ton_amount = body.load_coins()
        self.refund_address = body.load_address()
        self.forward_payload = body.load_maybe_ref()
        if not self.forward_payload and body.remaining_refs > 0:
            self.forward_payload = body.to_cell()



class StonfiV2PayTo:
    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.to_address = body.load_address()
        self.excesses_address = body.load_address()
        self.original_caller = body.load_address()
        self.exit_code = body.load_uint(32)
        self.custom_payload = body.load_maybe_ref()
        additional_info = body.load_ref().to_slice()
        self.fwd_ton_amount = additional_info.load_coins()
        self.amount0_out = additional_info.load_coins()
        self.token0_address = additional_info.load_address()
        self.amount1_out = additional_info.load_coins()
        self.token1_address = additional_info.load_address()


class DedustSwapNotification:
    opcode = 0x9c610de3

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.asset_in = load_asset(body)
        self.asset_out = load_asset(body)
        self.amount_in = body.load_coins()
        self.amount_out = body.load_coins()
        ref = body.load_ref().to_slice()
        self.sender_address = ref.load_address()
        self.ref_address = ref.load_address()
        self.reserve_0 = ref.load_coins()
        self.reserve_1 = ref.load_coins()


class DedustPayout:
    opcode = 0x474f86cf

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.proof = body.load_ref()
        self.amount = body.load_coins()


class DedustPayoutFromPool:
    opcode = 0xad4eb6f5

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.proof = body.load_ref()
        self.amount = body.load_coins()

class DedustSwapPeer:
    opcode = 0x72aca8aa


class DedustSwapExternal:
    opcode = 0x61ee542d

class DedustSwap:
    opcode = 0xea06185d

class DedustSwapPayload:
    opcode = 0xe3a0d482

class StonfiSwapV2:
    opcode = 0x657b54f5
    # query_id: int
    # from_user: Address
    # left_amount: int
    # right_amount: int
    # transferred_op: int
    # token_wallet1: Address
    # refund_address: Address
    # excesses_address: str
    # tx_deadline: int
    # min_out: int
    # receiver: str
    # fwd_gas: int
    # custom_payload: bytes | None
    # refund_fwd_gas: int
    # refund_payload: bytes | None
    # ref_fee: int
    # ref_address: str

    def __init__(self, boc: Slice):
        boc.skip_bits(32)  # Skip opcode
        self.query_id = boc.load_uint(64)
        self.from_user = boc.load_address()
        self.left_amount = boc.load_coins()
        self.right_amount = boc.load_coins()
        dex_payload_slice = boc.load_ref().to_slice()
        self.transferred_op = dex_payload_slice.load_uint(32)
        self.token_wallet1 = dex_payload_slice.load_address()
        self.refund_address = dex_payload_slice.load_address()
        self.excesses_address = dex_payload_slice.load_address()
        self.tx_deadline = dex_payload_slice.load_uint(64)
        swap_body_slice = dex_payload_slice.load_ref().to_slice()
        self.min_out = swap_body_slice.load_coins()
        self.receiver = swap_body_slice.load_address()
        self.fwd_gas = swap_body_slice.load_coins()
        custom_payload = swap_body_slice.load_maybe_ref()
        self.custom_payload = None
        if custom_payload:
            self.custom_payload = custom_payload.to_boc(hash_crc32=True)
        self.refund_fwd_gas = swap_body_slice.load_coins()
        self.refund_payload = None
        refund_payload = swap_body_slice.load_maybe_ref()
        if refund_payload:
            self.refund_payload = refund_payload.to_boc(hash_crc32=True)
        self.ref_fee = swap_body_slice.load_uint(16)
        self.ref_address = swap_body_slice.load_address()

    def get_pool_accounts_recursive(self) -> list[str]:
        accounts = [self.token_wallet1.to_str(is_user_friendly=False).upper() if isinstance(self.token_wallet1, Address) else ""]
        if self.custom_payload is None:
            return accounts
        current_slice = Slice.one_from_boc(self.custom_payload)
        while True:
            sum_type = current_slice.load_uint(32)
            if sum_type in (0x6664de2a, 0x69cf1a5b):
                account = current_slice.load_address()
                accounts.append(account.to_str(is_user_friendly=False).upper() if isinstance(account, Address) else "")
                if current_slice.remaining_refs > 0:
                    cross_swap = current_slice.load_ref().to_slice()
                else:
                    break
                cross_swap.load_coins() # min_out
                cross_swap.load_coins()
                if cross_swap.remaining_refs > 0:
                    custom_payload = cross_swap.load_maybe_ref()
                    if custom_payload:
                        current_slice = custom_payload.to_slice()
                    else:
                        break
                else:
                    break
            else:
                break
        return accounts
    

# --- TONCO Router Messages ---

class ToncoRouterV3CreatePool:
    """
    Opcode: 0x2e3034ef
    TL-B:
    ROUTERV3_CREATE_POOL#2e3034ef 
        query_id:uint64
        jetton_wallet0:MsgAddress
        jetton_wallet1:MsgAddress
        tick_spacing:int24
        initial_price_x96:uint160
        protocol_fee:uint16
        lp_fee_base:uint16
        lp_fee_current:uint16
        nftv3_content:^Cell
        nftv3item_content:^Cell
        minter_cell:^[
            jetton0_minter:MsgAddress
            jetton1_minter:MsgAddress
            controller_addr:MsgAddress
        ]  
    = ContractMessages;
    """
    opcode = 0x2e3034ef

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.jetton_wallet0 = body.load_address()
        self.jetton_wallet1 = body.load_address()
        self.tick_spacing = body.load_int(24)
        self.initial_price_x96 = body.load_uint(160)
        self.protocol_fee = body.load_uint(16)
        self.lp_fee_base = body.load_uint(16)
        self.lp_fee_current = body.load_uint(16)
        self.nftv3_content = body.load_ref()
        self.nftv3item_content = body.load_ref()

        minter_cell_ref = body.load_ref()
        minter_slice = minter_cell_ref.to_slice()
        self.jetton0_minter = minter_slice.load_address()
        self.jetton1_minter = minter_slice.load_address()
        self.controller_addr = minter_slice.load_address()


class ToncoRouterV3PayTo:
    """
    Payload format for JETTON_TRANSFER_NOTIFICATION (0x7362d09c)
    Opcode: 0xa1daa96d
    TL-B:
    ROUTERV3_PAY_TO#a1daa96d
        query_id:uint64
        reciever0:MsgAddress
        reciever1:MsgAddress
        exit_code:uint32
        seqno:uint64
        coinsinfo_cell:(Maybe ^[
            amount0:(VarUInteger 16)
            jetton0_address:MsgAddress
            amount1:(VarUInteger 16)
            jetton1_address:MsgAddress
        ] ) 
        (exit_code = 200)?(
            indexer_swap_info_cell:(Maybe ^[
                liquidity:uint128
                price_sqrt:uint160
                tick:int24
                fee_growth_global_0x128:int256
                fee_growth_global_1x128:int256
            ] ) 
        )
        (exit_code = 201)?(
            indexer_burn_info_cell:(Maybe ^[
                nft_index:uint64
                liquidity_burned:uint128
                tick_lower:int24
                tick_upper:int24
                tick_burn:int24
            ] ) 
        )
    = ContractMessages;
    """
    opcode = 0xa1daa96d

    def __init__(self, body: Slice):
        body.load_uint(32) # payload opcode
        self.query_id = body.load_uint(64)
        self.receiver0 = body.load_address()
        self.receiver1 = body.load_address()
        self.exit_code = body.load_uint(32)
        self.seqno = body.load_uint(64)

        # coinsinfo_cell
        self.amount0 = None
        self.jetton0_address = None
        self.amount1 = None
        self.jetton1_address = None
        coinsinfo_cell_ref = body.load_maybe_ref()
        if coinsinfo_cell_ref:
            coinsinfo_slice = coinsinfo_cell_ref.to_slice()
            self.amount0 = coinsinfo_slice.load_coins() or 0
            self.jetton0_address = coinsinfo_slice.load_address()
            self.amount1 = coinsinfo_slice.load_coins() or 0
            self.jetton1_address = coinsinfo_slice.load_address()

        # indexer_swap_info_cell (conditional)
        self.liquidity = None
        self.price_sqrt = None
        self.tick_swap = None
        self.fee_growth_global_0x128 = None
        self.fee_growth_global_1x128 = None
        self.has_swap_info = False
        if self.exit_code == 200:
            self.has_swap_info = True
            indexer_swap_info_cell_ref = body.load_maybe_ref()
            if indexer_swap_info_cell_ref:
                indexer_swap_info_slice = indexer_swap_info_cell_ref.to_slice()
                self.liquidity = indexer_swap_info_slice.load_uint(128) or 0
                self.price_sqrt = indexer_swap_info_slice.load_uint(160)
                self.tick_swap = indexer_swap_info_slice.load_int(24)
                self.fee_growth_global_0x128 = indexer_swap_info_slice.load_int(256)
                self.fee_growth_global_1x128 = indexer_swap_info_slice.load_int(256)

        # indexer_burn_info_cell (conditional)
        self.nft_index = None
        self.liquidity_burned = None
        self.tick_lower = None
        self.tick_upper = None
        self.tick_burn = None
        self.has_burn_info = False
        if self.exit_code == 201:
            self.has_burn_info = True
            indexer_burn_info_cell_ref = body.load_maybe_ref()
            if indexer_burn_info_cell_ref:
                indexer_burn_info_slice = indexer_burn_info_cell_ref.to_slice()
                self.nft_index = indexer_burn_info_slice.load_uint(64)
                self.liquidity_burned = indexer_burn_info_slice.load_uint(128)
                self.tick_lower = indexer_burn_info_slice.load_int(24)
                self.tick_upper = indexer_burn_info_slice.load_int(24)
                self.tick_burn = indexer_burn_info_slice.load_int(24)

class ToncoResetGas:
    """
    Opcode: 0x42a0fb43
    TL-B:
    ROUTERV3_RESET_GAS#42a0fb43 
        query_id:uint64
    = ContractMessages;
    """
    opcode = 0x42a0fb43

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)


# --- TONCO Position NFT Messages ---

class ToncoPositionNftV3PositionInit:
    """
    Opcode: 0xd5ecca2a
    TL-B:
    POSITIONNFTV3_POSITION_INIT#d5ecca2a 
        query_id:uint64
        user_address:MsgAddress
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
        old_fee_cell:^[
            fee_growth_inside_0_last_x128:uint256
            fee_growth_inside_1_last_x128:uint256
            nft_index:uint64
            jetton0_amount:(VarUInteger 16)
            jetton1_amount:(VarUInteger 16)
            tick:int24
        ]  
    = ContractMessages;
    """
    opcode = 0xd5ecca2a

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.user_address = body.load_address()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

        old_fee_cell_ref = body.load_ref()
        old_fee_slice = old_fee_cell_ref.to_slice()
        self.fee_growth_inside0_last_x128 = old_fee_slice.load_uint(256)
        self.fee_growth_inside1_last_x128 = old_fee_slice.load_uint(256)
        self.nft_index = old_fee_slice.load_uint(64)
        self.jetton0_amount = old_fee_slice.load_coins()
        self.jetton1_amount = old_fee_slice.load_coins()
        self.tick = old_fee_slice.load_int(24)

class ToncoPositionNftV3PositionBurn:
    """
    Opcode: 0x46ca335a
    TL-B:
    POSITIONNFTV3_POSITION_BURN#46ca335a 
        query_id:uint64
        nft_owner:MsgAddress
        liquidity_to_burn:uint128
        tick_lower:int24
        tick_upper:int24
        old_fee_cell:^[
            fee_growth_inside_0_last_x128:uint256
            fee_growth_inside_1_last_x128:uint256
        ]  
    = ContractMessages;
    """
    opcode = 0x46ca335a

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.nft_owner = body.load_address()
        self.liquidity_to_burn = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

        old_fee_cell_ref = body.load_ref()
        old_fee_slice = old_fee_cell_ref.to_slice()
        self.fee_growth_inside0_last_x128 = old_fee_slice.load_uint(256)
        self.fee_growth_inside1_last_x128 = old_fee_slice.load_uint(256)


# --- TONCO Pool Messages ---

class ToncoPoolV3Init:
    """
    POOLV3_INIT#441c39ed
        query_id:uint64
        from_admin:bool
        has_admin:bool
        admin_addr:MsgAddress
        has_controller:bool
        controller_addr:MsgAddress
        set_spacing:bool
        tick_spacing:int24
        set_price:bool
        initial_price_x96:uint160
        set_active:bool
        pool_active:bool
        protocol_fee:uint16
        lp_fee_base:uint16
        lp_fee_current:uint16
        nftv3_content:^Cell
        nftv3item_content:^Cell
        minter_cell:(Maybe ^[  
            jetton0_minter:MsgAddress
            jetton1_minter:MsgAddress
        ])
    = ContractMessages;
    """
    opcode = 0x441c39ed

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.from_admin = body.load_bit()
        self.has_admin = body.load_bit()
        self.admin_addr = body.load_address()
        self.has_controller = body.load_bit()
        self.controller_addr = body.load_address()
        self.set_spacing = body.load_bit()
        self.tick_spacing = body.load_int(24)
        self.set_price = body.load_bit()
        self.initial_price_x96 = body.load_uint(160)
        self.set_active = body.load_bit()
        self.pool_active = body.load_bit()
        self.protocol_fee = body.load_uint(16)
        self.lp_fee_base = body.load_uint(16)
        self.lp_fee_current = body.load_uint(16)
        self.nftv3_content = body.load_ref()
        self.nftv3item_content = body.load_ref()
        minter_cell_ref = body.load_maybe_ref()
        self.jetton0_minter = None
        self.jetton1_minter = None
        if minter_cell_ref:
            minter_slice = minter_cell_ref.to_slice()
            self.jetton0_minter = minter_slice.load_address()
            self.jetton1_minter = minter_slice.load_address()

class ToncoPoolV3Lock:
    """
    POOLV3_LOCK#b1b0b7e2
        query_id:uint64
    = ContractMessages;
    """
    opcode = 0xb1b0b7e2
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)

class ToncoPoolV3Unlock:
    """
    POOLV3_UNLOCK#4e737e4d
        query_id:uint64
    = ContractMessages;
    """
    opcode = 0x4e737e4d
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)

class ToncoPoolV3Mint:
    """
    POOLV3_MINT#b2c1b6e3
        query_id:uint64
        owner_addr:MsgAddress
        amount0:(VarUInteger 16)
        amount1:(VarUInteger 16)
        enough0:(VarUInteger 16)
        enough1:(VarUInteger 16)
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """
    opcode = 0xb2c1b6e3
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.owner_addr = body.load_address()
        self.amount0 = body.load_coins()
        self.amount1 = body.load_coins()
        self.enough0 = body.load_coins()
        self.enough1 = body.load_coins()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

class ToncoPoolV3MinAndRefund:
    """
    POOLV3_MINT#81702ef8
        query_id:uint64
        amount0_funded:(VarUInteger 16)
        amount1_funded:(VarUInteger 16)
        recipient:MsgAddress
        liquidity:uint128
        tickLower:int24
        tickUpper:int24
    = ContractMessages;
    """
    opcode = 0x81702ef8
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount0_funded = body.load_coins()
        self.amount1_funded = body.load_coins()
        self.recipient = body.load_address()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

class ToncoPoolV3Burn:
    """
    POOLV3_BURN#d73ac09d
        query_id:uint64
        recipient:MsgAddress
        burned_index:uint64
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
        liquidity_to_burn:uint128
        old_fee_cell:^[
            fee_growth_inside_0_last_x128:uint256
            fee_growth_inside_1_last_x128:uint256
        ]
        new_fee_cell:^[
            fee_growth_inside_0_current_x128:uint256
            fee_growth_inside_1_current_x128:uint256
        ]
    = ContractMessages;
    """
    opcode = 0xd73ac09d
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.recipient = body.load_address()
        self.burned_index = body.load_uint(64)
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)
        self.liquidity_to_burn = body.load_uint(128)
        old_fee_cell = body.load_ref().to_slice()
        self.fee_growth_inside_0_last_x128 = old_fee_cell.load_uint(256)
        self.fee_growth_inside_1_last_x128 = old_fee_cell.load_uint(256)
        new_fee_cell = body.load_ref().to_slice()
        self.fee_growth_inside_0_current_x128 = new_fee_cell.load_uint(256)
        self.fee_growth_inside_1_current_x128 = new_fee_cell.load_uint(256)

class ToncoPoolV3SetFee:
    """
    POOLV3_SET_FEE#6bdcbeb8
        query_id:uint64
        protocol_fee:uint16
        lp_fee_base:uint16
        lp_fee_current:uint16
    = ContractMessages;
    """
    opcode = 0x6bdcbeb8
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.protocol_fee = body.load_uint(16)
        self.lp_fee_base = body.load_uint(16)
        self.lp_fee_current = body.load_uint(16)

class ToncoPoolV3FundAccount:
    """
    POOLV3_FUND_ACCOUNT#4468de77
        query_id:uint64
        owner_addr:MsgAddress
        amount0:(VarUInteger 16)
        amount1:(VarUInteger 16)
        enough0:(VarUInteger 16)
        enough1:(VarUInteger 16)
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """
    opcode = 0x4468de77
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.owner_addr = body.load_address()
        self.amount0 = body.load_coins()
        self.amount1 = body.load_coins()
        self.enough0 = body.load_coins()
        self.enough1 = body.load_coins()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

class ToncoPoolV3StartBurn:
    """
    POOLV3_START_BURN#530b5f2c
        query_id:uint64
        burned_index:uint64
        liquidity_to_burn:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """
    opcode = 0x530b5f2c
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.burned_index = body.load_uint(64)
        self.liquidity_to_burn = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

class ToncoPoolV3Swap:
    """
    POOLV3_SWAP#a7fb58f8
        query_id:uint64
        owner_address:MsgAddress
        source_wallet:MsgAddress
        params_cell:^[
            amount:(VarUInteger 16)
            sqrt_price_limit_x96:uint160
            min_out_amount:(VarUInteger 16)
        ]
        payloads_cell:^[
            target_address:MsgAddress
            ok_forward_amount:(VarUInteger 16)
            ok_forward_payload:(Maybe ^Cell)
            ret_forward_amount:(VarUInteger 16)
            ret_forward_payload:(Maybe ^Cell)
        ]
    = ContractMessages;
    """
    opcode = 0xa7fb58f8
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.owner_address = body.load_address()
        self.source_wallet = body.load_address()
        params_cell = body.load_ref().to_slice()
        self.amount = params_cell.load_coins() or 0
        self.sqrt_price_limit_x96 = params_cell.load_uint(160)
        self.min_out_amount = params_cell.load_coins() or 0
        payloads_cell = body.load_ref().to_slice()
        self.target_address = payloads_cell.load_address()
        self.ok_forward_amount = payloads_cell.load_coins() or 0
        self.ok_forward_payload = payloads_cell.load_maybe_ref()
        self.ret_forward_amount = payloads_cell.load_coins() or 0
        self.ret_forward_payload = payloads_cell.load_maybe_ref()
    
class ToncoPoolV3SwapPayload:
    """With reason most obscure and purpose veiled in shadow,
    tonco does employ the selfsame opcode for in_transfer payload, yet
    with form so strange and foreign, not once described in scrolls of documentation.
    Thus, like ancient runes deciphered, here lie message builders from the mystical 'eir sdk:

    ```ts
    swapRequest = beginCell()
      .storeUint(ContractOpcodes.POOLV3_SWAP, 32) // Request to swap
      .storeAddress(routerJettonWallet) // JettonWallet attached to Router is used to identify target token
      .storeUint(priceLimitSqrt, 160) // Minimum/maximum price that we are ready to reach
      .storeCoins(minimumAmountOut) // Minimum amount to get back
      .storeAddress(recipient) // Address to receive result of the swap
      .storeUint(0, 1) // Payload Maybe Ref // Address to recieve result of the swap
      .endCell();
    const multicallMessage = beginCell()
        .storeUint(ContractOpcodes.POOLV3_SWAP, 32)
        .storeAddress(jettonRouterWallet)
        .storeUint(priceLimitSqrt || BigInt(0), 160)
        .storeCoins(minimumAmountOut || BigInt(0))
        .storeAddress(recipient)
        .storeMaybeRef(getInnerMessage(isEmpty, Boolean(isPTON)))
        .endCell();
    ```"""
    payload_opcode = 0xa7fb58f8
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.target_router_jwallet = body.load_address()
        self.price_limit_sqrt = body.load_uint(160)
        self.min_out_amount = body.load_coins() or 0
        self.recipient = body.load_address()
        self.payload = body.load_maybe_ref()
    
    def get_target_wallets_and_amounts_recursive(self) -> list[tuple[str, int]]:
        accounts = []
        
        if isinstance(self.target_router_jwallet, Address):
            accounts.append((self.target_router_jwallet.to_str(False).upper(), self.min_out_amount))
        
        if self.payload:
            try:
                inner_payload = self.payload.to_slice()
                op = inner_payload.preload_uint(32)
                if op == self.payload_opcode:
                    next_payload = ToncoPoolV3SwapPayload(inner_payload)
                    accounts.extend(next_payload.get_target_wallets_and_amounts_recursive())
            except Exception:
                pass
        return accounts
    
    def get_target_wallets_recursive(self) -> set[str]:
        return set([account[0] for account in self.get_target_wallets_and_amounts_recursive()])

class ToncoPoolV3FundAccountPayload:
    """
    Payload for jetton notification during liquidity provision.
    Used inside jetton transfers to router for mint operations.
    TL-B structure from SDK:
    POOLV3_FUND_ACCOUNT#4468de77
        other_jetton_wallet:MsgAddress    // jetton wallet of the other token
        amount0:(VarUInteger 16)          // amount of first token
        amount1:(VarUInteger 16)          // amount of second token  
        liquidity:uint128                 // liquidity to provide
        tick_lower:int24                  // lower tick
        tick_upper:int24                  // upper tick
    = ContractMessages;
    """
    payload_opcode = 0x4468de77

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.other_jetton_wallet = body.load_address()
        self.amount0 = body.load_coins() or 0
        self.amount1 = body.load_coins() or 0
        # self.liquidity = body.load_uint(128)
        # self.tick_lower = body.load_int(24)
        # self.tick_upper = body.load_int(24)
    
    def get_other_jetton_wallet(self) -> str:
        if not isinstance(self.other_jetton_wallet, Address):
            raise ValueError("other_jetton_wallet is not an Address")
        return self.other_jetton_wallet.to_str(False).upper()

# --- TONCO Account Messages ---

class ToncoAccountV3AddLiquidity:
    """
    Opcode: 0x3ebe5431
    TL-B:
    ACCOUNTV3_ADD_LIQUIDITY#3ebe5431 
        query_id:uint64
        new_amount0:(VarUInteger 16)
        new_amount1:(VarUInteger 16)
        new_enough0:(VarUInteger 16)
        new_enough1:(VarUInteger 16)
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """
    opcode = 0x3ebe5431

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.new_amount0 = body.load_coins() or 0
        self.new_amount1 = body.load_coins() or 0
        self.new_enough0 = body.load_coins() or 0
        self.new_enough1 = body.load_coins() or 0
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)


class ToncoAccountV3RefundMe:
    """
    Opcode: 0xbf3f447
    TL-B:
    ACCOUNTV3_REFUND_ME#bf3f447 
        query_id:uint64
    = ContractMessages;
    """
    opcode = 0xbf3f447

    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)