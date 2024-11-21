from pytoniq_core import Slice

from indexer.events.blocks.utils import AccountId


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
