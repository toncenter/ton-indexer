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