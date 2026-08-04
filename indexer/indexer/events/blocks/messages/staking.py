from pytoniq_core import Slice, Address

from indexer.events.blocks.utils import AccountId

class ElectorRecoverStakeRequest:
    opcode = 0x47657424

class ElectorRecoverStakeConfirmation:
    opcode = 0xf96f7324

class ElectorDepositStakeConfirmation:
    opcode = 0xf374484c

class ElectorDepositStakeRequest:
    opcode = 0x4e73744b

class TONStakersDepositRequest:
    opcode = 0x47D54391


class TONStakersWithdrawRequest:
    opcode = 0x319B0CDC


class TONStakersMintJettons:
    opcode = 0x1674B0A0


class TONStakersMintNFT:
    opcode = 0x1674B0A0


class TONStakersInitNFT:
    opcode = 0x132F9A45

class TONStakersNftBurn:
    opcode = 0xF127FE4E

class TONStakersNftBurnNotification:
    opcode = 0xED58B0B2

    query_id: int
    amount: int
    owner: Address

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_coins()
        self.owner = slice.load_address()

# Payout after nft burnt
class TONStakersDistributedAsset:
    opcode = 0xDB3B8ABD

class TONStakersPoolWithdrawal:
    opcode = 0x0A77535C

class NominatorPoolProcessWithdrawRequests:
    opcode = 0x00000002

# ---------------------------------------------------------------------------
# Hipo (hGRAM liquid staking) — https://hipo.finance
#
# Op-codes and field layouts are taken from the protocol's TL-B schema:
# https://github.com/HipoFinance/contract/blob/main/contracts/schema.tlb
#
# The treasury address is the stable anchor of the protocol: user wallets and
# the jetton parent may be upgraded (the treasury keeps a list of `old_parents`),
# but the treasury address never changes, so matchers key on it.
# ---------------------------------------------------------------------------

HIPO_TREASURY_ADDRESS = "0:8BC991CFE177BC7E9721433EFA3BEFD199485A55CFFD040A06C89AF026B71BCF"
HIPO_PARENT_ADDRESS = "0:CF76AF318C0872B58A9F1925FC29C156211782B9FB01F56760D292E56123BF87"


class HipoDepositCoins:
    """staker -> treasury: deposit GRAM and ask for hGRAM."""

    opcode = 0x3D3761A6

    query_id: int
    owner: Address | None
    coins: int
    ownership_assigned_amount: int
    referrer: Address | None

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.owner = slice.load_address()
        self.coins = slice.load_coins()
        self.ownership_assigned_amount = slice.load_coins()
        self.referrer = slice.load_address()


class HipoProxyTokensMinted:
    """treasury -> parent: hGRAM minted for `owner`."""

    opcode = 0x5BE57626

    query_id: int
    tokens: int
    coins: int
    owner: Address | None
    round_since: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.coins = slice.load_coins()
        self.owner = slice.load_address()
        self.round_since = slice.load_uint(32)


class HipoTokensMinted:
    """parent -> hGRAM wallet: credit the minted hGRAM."""

    opcode = 0x5445EFEE

    query_id: int
    tokens: int
    coins: int
    owner: Address | None
    round_since: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.coins = slice.load_coins()
        self.owner = slice.load_address()
        self.round_since = slice.load_uint(32)


class HipoProxySaveCoins:
    """treasury -> parent: remember a deposit that is pending until round end."""

    opcode = 0x47DAA10F

    query_id: int
    coins: int
    owner: Address | None
    round_since: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.coins = slice.load_coins()
        self.owner = slice.load_address()
        self.round_since = slice.load_uint(32)


class HipoSaveCoins:
    """parent -> hGRAM wallet: remember a pending deposit."""

    opcode = 0x4CCE0E74

    query_id: int
    coins: int
    owner: Address | None
    round_since: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.coins = slice.load_coins()
        self.owner = slice.load_address()
        self.round_since = slice.load_uint(32)


class HipoMintBill:
    """treasury -> bill collection: mint the SBT that tracks a pending request."""

    opcode = 0x4B2D7871

    query_id: int
    amount: int
    unstake: bool
    owner: Address | None
    parent: Address | None
    ownership_assigned_amount: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_coins()
        self.unstake = bool(slice.load_bit())
        self.owner = slice.load_address()
        self.parent = slice.load_address()
        self.ownership_assigned_amount = slice.load_coins()


class HipoAssignBill:
    """bill collection -> bill: initialize the SBT."""

    opcode = 0x3275DFC2

    query_id: int
    amount: int
    unstake: bool
    owner: Address | None
    parent: Address | None
    ownership_assigned_amount: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_coins()
        self.unstake = bool(slice.load_bit())
        self.owner = slice.load_address()
        self.parent = slice.load_address()
        self.ownership_assigned_amount = slice.load_coins()


class HipoProxyReserveTokens:
    """hGRAM wallet -> parent: forward an unstake request."""

    opcode = 0x688B0213

    query_id: int
    tokens: int
    owner: Address | None
    mode: int
    ownership_assigned_amount: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.owner = slice.load_address()
        self.mode = slice.load_uint(4)
        self.ownership_assigned_amount = slice.load_coins()


class HipoReserveTokens:
    """parent -> treasury: unstake request lands on the treasury."""

    opcode = 0x386A358B

    query_id: int
    tokens: int
    owner: Address | None
    mode: int
    ownership_assigned_amount: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.owner = slice.load_address()
        self.mode = slice.load_uint(4)
        self.ownership_assigned_amount = slice.load_coins()


class HipoProxyTokensBurned:
    """treasury -> parent: hGRAM burned, `coins` GRAM are on their way back."""

    opcode = 0x4476FDE0

    query_id: int
    tokens: int
    coins: int
    owner: Address | None

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.coins = slice.load_coins()
        self.owner = slice.load_address()


class HipoTokensBurned:
    """parent -> hGRAM wallet: hGRAM burned."""

    opcode = 0x5B512E25

    query_id: int
    tokens: int
    coins: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.coins = slice.load_coins()


class HipoWithdrawalNotification:
    """hGRAM wallet -> staker: carries the withdrawn GRAM."""

    opcode = 0xF0FA223B

    query_id: int
    tokens: int
    coins: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.coins = slice.load_coins()


class HipoProxyRollbackUnstake:
    """treasury -> parent: the unstake could not be served, give the hGRAM back."""

    opcode = 0x32B67194

    query_id: int
    tokens: int
    owner: Address | None

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.owner = slice.load_address()


class HipoRollbackUnstake:
    """parent -> hGRAM wallet: restore the hGRAM balance."""

    opcode = 0x1B77FD1A

    query_id: int
    tokens: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()


class HipoBurnBill:
    """bill collection -> bill: round ended, burn the SBT."""

    opcode = 0x6F89F5E3


class HipoBillBurned:
    """bill -> bill collection: the SBT is gone, settle the request."""

    opcode = 0x840F6369

    query_id: int
    amount: int
    unstake: bool
    owner: Address | None
    parent: Address | None
    index: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_coins()
        self.unstake = bool(slice.load_bit())
        self.owner = slice.load_address()
        self.parent = slice.load_address()
        self.index = slice.load_uint(64)


class HipoMintTokens:
    """bill collection -> treasury: settle a pending deposit at round end."""

    opcode = 0x42684479

    query_id: int
    coins: int
    owner: Address | None
    parent: Address | None
    round_since: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.coins = slice.load_coins()
        self.owner = slice.load_address()
        self.parent = slice.load_address()
        self.round_since = slice.load_uint(32)


class HipoBurnTokens:
    """bill collection -> treasury: settle a pending unstake at round end."""

    opcode = 0x7CFFE1EE

    query_id: int
    tokens: int
    owner: Address | None
    parent: Address | None
    round_since: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.tokens = slice.load_coins()
        self.owner = slice.load_address()
        self.parent = slice.load_address()
        self.round_since = slice.load_uint(32)
