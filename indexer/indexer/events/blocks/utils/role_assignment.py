from __future__ import annotations

from typing import Callable

from indexer.core.database import (
    Action, OBSERVER, ECON_OUT, ECON_IN, ECON_BOTH,
    INITIATOR, INIT_OUT, INIT_BOTH,
)
from indexer.events.blocks.elections import elector_address


def _default_role_assignment(action: Action) -> dict[str, int]:
    """Standard value transfer: source sends, destination receives, unnamed = ECON_BOTH."""
    initiator = action.source
    out_accounts = set()
    in_accounts = set()

    if action.source is not None:
        out_accounts.add(action.source)
    if action.source_secondary is not None:
        out_accounts.add(action.source_secondary)
    if action.destination is not None:
        in_accounts.add(action.destination)
    if action.destination_secondary is not None:
        in_accounts.add(action.destination_secondary)

    roles = {}
    for account in action.accounts:
        role = 0
        if account in out_accounts:
            role |= ECON_OUT
        if account in in_accounts:
            role |= ECON_IN
        if account == initiator:
            role |= INITIATOR
        if role == 0:
            role = ECON_BOTH
        roles[account] = role
    return roles


def _governance_role_assignment(action: Action) -> dict[str, int]:
    """No value flow. Initiator triggers, everything else is observer."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INITIATOR
        else:
            roles[account] = OBSERVER
    return roles


def _swap_role_assignment(action: Action) -> dict[str, int]:
    """Swap initiator sends and receives (7). Pools are ECON_BOTH (3). Rest OBSERVER (0)."""
    initiator = action.source

    # Named directional accounts (excluding initiator, handled separately)
    named_out = set()
    named_in = set()
    if action.source_secondary is not None:
        named_out.add(action.source_secondary)
    if action.destination is not None:
        named_in.add(action.destination)
    if action.destination_secondary is not None:
        named_in.add(action.destination_secondary)

    # Pool accounts from composite data
    pool_accounts = set()
    if action.jetton_swap_data is not None:
        inc = action.jetton_swap_data.get('dex_incoming_transfer', {})
        out = action.jetton_swap_data.get('dex_outgoing_transfer', {})
        for field in ('destination', 'destination_jetton_wallet', 'source', 'source_jetton_wallet'):
            for transfer in (inc, out):
                addr = transfer.get(field)
                if addr is not None:
                    pool_accounts.add(addr)

    roles = {}
    for account in action.accounts:
        if account == initiator:
            roles[account] = INIT_BOTH
        elif account in named_out:
            roles[account] = ECON_OUT
        elif account in named_in:
            roles[account] = ECON_IN
        elif account in pool_accounts:
            roles[account] = ECON_BOTH
        else:
            roles[account] = OBSERVER
    return roles


def _staking_deposit_role_assignment(action: Action) -> dict[str, int]:
    """Staking deposit/withdrawal-request. Provider determines behavior:
    - tonstakers/ethena: user sends assets AND receives tokens/NFT back → INIT_BOTH(7)
    - nominator deposit: user sends TON, nothing back in same action → INIT_OUT(5)
    - nominator withdrawal_request: pure governance, no economic flow → INITIATOR(4)
    """
    provider = None
    if action.staking_data:
        provider = action.staking_data.get('provider')

    if provider == 'nominator' and action.type == 'stake_withdrawal_request':
        return _governance_role_assignment(action)

    source_role = INIT_BOTH if provider != 'nominator' else INIT_OUT

    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = source_role
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif account == action.destination:
            roles[account] = ECON_IN
        elif account == action.destination_secondary:
            roles[account] = ECON_IN
        else:
            roles[account] = ECON_BOTH
    return roles


def _withdrawal_role_assignment(action: Action) -> dict[str, int]:
    """User initiates and receives back (7). Counterparty pays out (1)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_BOTH
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif account == action.destination:
            roles[account] = ECON_OUT
        elif account == action.destination_secondary:
            roles[account] = ECON_OUT
        else:
            roles[account] = OBSERVER
    return roles


def _deposit_liquidity_role_assignment(action: Action) -> dict[str, int]:
    """User sends assets and receives LP (7). Pool is bidirectional (3)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_BOTH
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif account == action.destination:
            roles[account] = ECON_BOTH
        elif account == action.destination_secondary:
            roles[account] = OBSERVER
        else:
            roles[account] = ECON_BOTH
    return roles


def _purchase_role_assignment(action: Action) -> dict[str, int]:
    """Buyer (dest) = INIT_BOTH(7), Seller (src) = ECON_BOTH(3)."""
    roles = {}
    for account in action.accounts:
        if account == action.destination:
            roles[account] = INIT_BOTH
        elif account == action.source:
            roles[account] = ECON_BOTH
        else:
            roles[account] = OBSERVER
    return roles


def _election_deposit_role_assignment(action: Action) -> dict[str, int]:
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_OUT
        elif account == elector_address:
            roles[account] = ECON_IN
        else:
            roles[account] = OBSERVER
    return roles


def _election_recover_role_assignment(action: Action) -> dict[str, int]:
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_BOTH
        elif account == elector_address:
            roles[account] = ECON_OUT
        else:
            roles[account] = OBSERVER
    return roles


def _jetton_burn_role_assignment(action: Action) -> dict[str, int]:
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_OUT
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif action.asset and account == action.asset:
            roles[account] = ECON_IN
        else:
            roles[account] = OBSERVER
    return roles


def _jetton_mint_role_assignment(action: Action) -> dict[str, int]:
    """Source sends TON to master, master mints jettons to destination.
    When source == destination (common): source gets INIT_BOTH(7)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            # Source sent TON to the master to trigger the mint
            if account == action.destination:
                roles[account] = INIT_BOTH  # self-mint: initiated + sent TON + received jettons
            else:
                roles[account] = INIT_OUT   # sent TON to mint for someone else
        elif action.asset and account == action.asset:
            roles[account] = ECON_BOTH      # master: receives TON, emits jettons
        elif account == action.destination:
            roles[account] = ECON_IN
        elif account == action.destination_secondary:
            roles[account] = ECON_IN
        else:
            roles[account] = OBSERVER
    return roles


def _nft_mint_role_assignment(action: Action) -> dict[str, int]:
    """Deployer triggers (4), NFT item receives (2)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INITIATOR
        elif account == action.destination:
            roles[account] = ECON_IN
        else:
            roles[account] = OBSERVER
    return roles


def _evaa_withdraw_role_assignment(action: Action) -> dict[str, int]:
    """Owner initiates + receives (7). Recipient ECON_IN (2). EVAA contract pays out (1)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_BOTH
        elif account == action.destination:
            roles[account] = ECON_IN
        elif account == action.destination_secondary:
            roles[account] = ECON_OUT
        else:
            roles[account] = OBSERVER
    return roles


def _evaa_liquidate_role_assignment(action: Action) -> dict[str, int]:
    """Liquidator pays debt + receives collateral (7). Borrower loses collateral (1)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_BOTH
        elif account in (action.destination, action.destination_secondary):
            roles[account] = ECON_OUT
        else:
            roles[account] = OBSERVER
    return roles


def _tgbtc_mint_role_assignment(action: Action) -> dict[str, int]:
    """Source triggers (4), teleport emits (1), recipient receives (2)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INITIATOR
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif account in (action.destination, action.destination_secondary):
            roles[account] = ECON_IN
        else:
            roles[account] = OBSERVER
    return roles


def _tgbtc_burn_role_assignment(action: Action) -> dict[str, int]:
    """Burner sends (5), wallet sends (1), pegout is OBSERVER (0)."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_OUT
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif account == action.destination:
            roles[account] = OBSERVER
        else:
            roles[account] = OBSERVER
    return roles


def _coffee_staking_withdraw_role_assignment(action: Action) -> dict[str, int]:
    """Withdrawer receives tokens. Pool pays out. Wallet directions FLIPPED."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_BOTH
        elif account == action.source_secondary:
            roles[account] = ECON_IN
        elif account == action.destination:
            roles[account] = ECON_OUT
        elif account == action.destination_secondary:
            roles[account] = ECON_OUT
        else:
            roles[account] = OBSERVER
    return roles


def _auction_outbid_role_assignment(action: Action) -> dict[str, int]:
    """Auction refunds prev bidder. New bidder triggered it."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = ECON_OUT
        elif account == action.destination:
            roles[account] = ECON_IN
        elif account == action.source_secondary:
            roles[account] = INITIATOR
        else:
            roles[account] = OBSERVER
    return roles


def _vesting_send_role_assignment(action: Action) -> dict[str, int]:
    """Sender -> INIT_OUT, vesting contract -> OBSERVER, message dest -> ECON_IN."""
    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_OUT
        elif account == action.destination_secondary:
            roles[account] = ECON_IN
        elif account == action.destination:
            roles[account] = OBSERVER
        else:
            roles[account] = OBSERVER
    return roles


def _layerzero_send_role_assignment(action: Action) -> dict[str, int]:
    """Initiator sends. Endpoint receives from composite data."""
    endpoint = None
    if action.layerzero_send_data is not None:
        endpoint = action.layerzero_send_data.get('endpoint')

    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INIT_OUT
        elif account == action.source_secondary:
            roles[account] = ECON_OUT
        elif account in (action.destination, action.destination_secondary):
            roles[account] = ECON_IN
        elif endpoint and account == endpoint:
            roles[account] = ECON_IN
        else:
            roles[account] = OBSERVER
    return roles


def _layerzero_infra_role_assignment(action: Action) -> dict[str, int]:
    """LayerZero infrastructure: source and destination both INITIATOR."""
    dvn = None
    if action.layerzero_dvn_verify_data is not None:
        dvn = action.layerzero_dvn_verify_data.get('dvn')

    roles = {}
    for account in action.accounts:
        if account == action.source:
            roles[account] = INITIATOR
        elif account == action.destination:
            roles[account] = INITIATOR
        elif dvn and account == dvn:
            roles[account] = INITIATOR
        else:
            roles[account] = OBSERVER
    return roles


_custom_role_functions: dict[str, Callable[[Action], dict[str, int]]] = {
    # Swaps
    'jetton_swap': _swap_role_assignment,

    # Staking deposits (user sends assets + receives tokens/NFT back in merged action)
    'stake_deposit': _staking_deposit_role_assignment,
    'stake_withdrawal_request': _staking_deposit_role_assignment,

    # Withdrawals
    'stake_withdrawal': _withdrawal_role_assignment,
    'dex_withdraw_liquidity': _withdrawal_role_assignment,
    'tonco_withdraw_liquidity': _withdrawal_role_assignment,
    'jvault_unstake': _withdrawal_role_assignment,
    'jvault_claim': _withdrawal_role_assignment,
    'nft_cancel_sale': _withdrawal_role_assignment,
    'nft_cancel_auction': _withdrawal_role_assignment,
    'nft_finish_auction': _withdrawal_role_assignment,
    'teleitem_cancel_auction': _withdrawal_role_assignment,

    # Deposit liquidity
    'dex_deposit_liquidity': _deposit_liquidity_role_assignment,
    'dedust_deposit_liquidity': _deposit_liquidity_role_assignment,
    'dedust_deposit_liquidity_partial': _deposit_liquidity_role_assignment,
    'tonco_deposit_liquidity': _deposit_liquidity_role_assignment,
    'coffee_deposit_liquidity': _deposit_liquidity_role_assignment,

    # Purchases
    'nft_purchase': _purchase_role_assignment,
    'dns_purchase': _purchase_role_assignment,

    # Elections
    'election_deposit': _election_deposit_role_assignment,
    'election_recover': _election_recover_role_assignment,

    # Jetton burn/mint
    'jetton_burn': _jetton_burn_role_assignment,
    'jetton_mint': _jetton_mint_role_assignment,

    # NFT mint
    'nft_mint': _nft_mint_role_assignment,

    # EVAA
    'evaa_withdraw': _evaa_withdraw_role_assignment,
    'evaa_liquidate': _evaa_liquidate_role_assignment,

    # TgBTC
    'tgbtc_mint': _tgbtc_mint_role_assignment,
    'tgbtc_mint_fallback': _tgbtc_mint_role_assignment,
    'tgbtc_burn': _tgbtc_burn_role_assignment,
    'tgbtc_burn_fallback': _tgbtc_burn_role_assignment,

    # Coffee staking
    'coffee_staking_withdraw': _coffee_staking_withdraw_role_assignment,

    # Auction
    'auction_outbid': _auction_outbid_role_assignment,

    # Vesting
    'vesting_send_message': _vesting_send_role_assignment,

    # LayerZero
    'layerzero_send': _layerzero_send_role_assignment,
    'layerzero_send_tokens': _layerzero_send_role_assignment,
    'layerzero_receive': _layerzero_infra_role_assignment,
    'layerzero_commit_packet': _layerzero_infra_role_assignment,
    'layerzero_dvn_verify': _layerzero_infra_role_assignment,

    # Governance (no value flow)
    'tick_tock': _governance_role_assignment,
    'change_dns': _governance_role_assignment,
    'delete_dns': _governance_role_assignment,
    'unsubscribe': _governance_role_assignment,
    'nft_update_sale': _governance_role_assignment,
    'nft_discovery': _governance_role_assignment,
    'multisig_create_order': _governance_role_assignment,
    'multisig_approve': _governance_role_assignment,
    'multisig_execute': _governance_role_assignment,
    'vesting_add_whitelist': _governance_role_assignment,
    'jvault_unstake_request': _governance_role_assignment,
    'tgbtc_new_key': _governance_role_assignment,
    'tgbtc_new_key_fallback': _governance_role_assignment,
    'tgbtc_dkg_log_fallback': _governance_role_assignment,
}


def assign_roles(action: Action) -> dict[str, int]:
    """Entry point. Dispatches to custom or default handler by action.type."""
    handler = _custom_role_functions.get(action.type, _default_role_assignment)
    return handler(action)
