"""Shared `produces`-name normalization.

`produces` in .mch names either a host block CLASS (PascalCase, e.g.
`JettonTransferBlock`) or a bare btype string (snake_case, e.g. `jetton_transfer`).
IR emission and the declarative build path both need the
btype STRING the produced block actually carries, so the class -> btype map
lives here.

`produces_btype` is a pure name lookup with an identity fallback: a snake_case
name IS its btype, and a PascalCase class name resolves through `CLASS_TO_BTYPE`.
It stays lenient (never raises) so emission is total even on synthetic test
matchers whose produced btype legitimately equals the name.

For a declarative matcher, an unmapped PascalCase name would become the
produced btype string and silently miss consumers keyed by the real snake_case
btype. The resolver reports `R022_UNNORMALIZED_PRODUCES_CLASS` for that case.
The check is scoped to
declarative matchers: a matcher with a host `build` fn constructs its own block,
so its `produces` btype is documentation only and an identity fallback is
harmless there.

The table is curated because emission runs on stub registries
(`ir_emit.build_stub_registries`, block_types ->
`object`) to stay off the heavy block/DB import stack, so the produced class's
real instance btype is not available at emit time. Only
PascalCase class names used by declarative matchers need an entry; snake_case
producers and builder-matcher producers bypass the table.
"""
from __future__ import annotations

CLASS_TO_BTYPE: dict[str, str] = {
    "JettonTransferBlock": "jetton_transfer",
    "JettonBurnBlock": "jetton_burn",
    "JettonMintBlock": "jetton_mint",
    "JettonSwapBlock": "jetton_swap",
    "LayerZeroReceiveBlock": "layerzero_receive",
    "LayerZeroSendBlock": "layerzero_send",
    "LayerZeroSendTokensBlock": "layerzero_send_tokens",
    "LayerZeroCommitPacketBlock": "layerzero_commit_packet",
    "LayerZeroDvnVerifyBlock": "layerzero_dvn_verify",
    "EvaaSupplyBlock": "evaa_supply",
    "EvaaWithdrawBlock": "evaa_withdraw",
    "TONStakersWithdrawBlock": "tonstakers_withdraw",
    "TONStakersWithdrawRequestBlock": "tonstakers_withdraw_request",
    "EthenaDepositBlock": "ethena_deposit",
    "VestingSendMessageBlock": "vesting_send_message",
    "ToncoDepositLiquidityBlock": "tonco_deposit_liquidity",
    "CoffeeCreateVaultBlock": "coffee_create_vault",
    "DedustDepositLiquidity": "dedust_deposit_liquidity",
    "DedustDepositLiquidityPartial": "dedust_deposit_liquidity_partial",
    "NftPurchaseBlock": "nft_purchase",
    "NftTransferBlock": "nft_transfer",
    "TgBTCMintBlock": "tgbtc_mint",
}


def is_class_name(name: str) -> bool:
    """A `produces` token is a class name iff it is PascalCase. btype
    names are snake_case and pass through `produces_btype` unchanged."""
    return bool(name) and name[0].isupper()


def produces_btype(name: str) -> str:
    return CLASS_TO_BTYPE.get(name, name)
