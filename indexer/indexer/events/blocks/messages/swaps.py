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

    opcode = 0xA7FB58F8

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

    payload_opcode = 0xA7FB58F8

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
            accounts.append(
                (self.target_router_jwallet.to_str(False).upper(), self.min_out_amount)
            )

        if self.payload:
            try:
                inner_payload = self.payload.to_slice()
                op = inner_payload.preload_uint(32)
                if op == self.payload_opcode:
                    next_payload = ToncoPoolV3SwapPayload(inner_payload)
                    accounts.extend(
                        next_payload.get_target_wallets_and_amounts_recursive()
                    )
            except Exception:
                pass
        return accounts

    def get_target_wallets_recursive(self) -> set[str]:
        return set(
            [account[0] for account in self.get_target_wallets_and_amounts_recursive()]
        )


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

    opcode = 0xA1DAA96D

    def __init__(self, body: Slice):
        body.load_uint(32)  # payload opcode
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

    def get_jetton_wallets(self) -> list[str]:
        jetton_wallets = []
        if isinstance(self.jetton0_address, Address):
            jetton_wallets.append(
                self.jetton0_address.to_str(is_user_friendly=False).upper()
            )
        if isinstance(self.jetton1_address, Address):
            jetton_wallets.append(
                self.jetton1_address.to_str(is_user_friendly=False).upper()
            )
        return jetton_wallets
