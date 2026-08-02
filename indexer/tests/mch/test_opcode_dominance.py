"""Closed manifest for host calls into the ABI-faithful message registry.

Prefixed ABI parsers check their own wire tag, so every host call must remain
dominated by the matching opcode. The prefixless JVault payload is the one
intentional exception: its first 32-bit word is retained as data.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from indexer.events.mch.ir_emit import emit_default


REPO = Path(__file__).resolve().parents[3]
HOST = REPO / "ton-index-worker/ton-mch-engine/src/host"


@dataclass(frozen=True)
class HostCall:
    file: str
    ordinal: int
    message: str
    guard_opcodes: tuple[int, ...]
    evidence: str


ABI_PREFIXES = {
    "EvaaSupplyMaster": 0x00000001,
    "EvaaSupplyJettonForward": 0x00000001,
    "EvaaSupplySuccess": 0x0000011A,
    "StonfiPaymentRequest": 0xF93BB43F,
    "StonfiSwapMessage": 0x25938561,
    "StonfiV2PayTo": 0x657B54F5,
    "PTonTransfer": 0x01F3835D,
    "ToncoRouterV3SwapSourceWallet": 0xA7FB58F8,
    "JettonNotify": 0x7362D09C,
    "DedustDepositLiquidityToPool": 0xB56B9598,
    "JettonInternalTransfer": 0x178D4519,
    "DedustSwapNotification": 0x9C610DE3,
    "DedustPayoutFromPool": 0xAD4EB6F5,
    "CoffeeSwapEvent": 0xC0FFEE30,
    "NftOwnershipAssignedPrevOwner": 0x05138D91,
    "JVaultStakePeriodPayload": None,
}


# Each entry was traced through its enclosing function and, where necessary,
# every helper caller / matcher capture. The ordinal is the 1-indexed occurrence
# count of this message within this file, not a line number. The empty guard
# belongs only to the honest prefixless payload row.
HOST_CALLS = (
    HostCall("HostEvaa.cpp", 1, "EvaaSupplyMaster", (0x00000001,),
             "evaa_supply_anchor accepts the call arm only through is_call_op(kSupplyMaster)"),
    HostCall("HostEvaa.cpp", 1, "EvaaSupplyJettonForward", (0x00000001,),
             "evaa_supply_anchor opens this same forward-payload cell and checks kSupplyMaster"),
    HostCall("HostEvaa.cpp", 1, "EvaaSupplySuccess", (0x0000011A,),
             "success is assigned only by is_call_op(kSupplySuccess)"),
    HostCall("HostStonfi.cpp", 1, "StonfiPaymentRequest", (0xF93BB43F,),
             "both helper callers use the StonfiPaymentRequest op node/is_call_op filter"),
    HostCall("HostStonfi.cpp", 1, "StonfiSwapMessage", (0x25938561,),
             "the helper input is the matcher's kStonfiSwap contract capture"),
    HostCall("HostStonfi.cpp", 1, "StonfiV2PayTo", (0x657B54F5,),
             "the helper input is selected from kV2PayTo contract calls"),
    HostCall("HostStonfi.cpp", 1, "PTonTransfer", (0x01F3835D,),
             "pton_self_transfer explicitly checks the body message opcode before parse"),
    HostCall("HostStonfi.cpp", 2, "PTonTransfer", (0x01F3835D,),
             "same branch checks im->opcode32() == kPTonTransfer"),
    HostCall("HostStonfi.cpp", 3, "PTonTransfer", (0x01F3835D,),
             "pton_transfer is assigned only by is_call_op(kPTonTransfer)"),
    HostCall("HostTonco.cpp", 1, "ToncoRouterV3SwapSourceWallet", (0xA7FB58F8,),
             "all helper inputs come from derive_tonco_parts kV3Swap filtering"),
    HostCall("HostTonco.cpp", 1, "JettonNotify", (0x7362D09C,),
             "jetton_notify_block is selected only by find_call/is_call_op(kJettonNotify)"),
    HostCall("HostTonco.cpp", 1, "PTonTransfer", (0x01F3835D,),
             "pton is returned by find_call(..., kPTonTransfer)"),
    HostCall("HostTonco.cpp", 2, "PTonTransfer", (0x01F3835D,),
             "pton is returned by find_call(..., kPTonTransfer)"),
    HostCall("HostTonco.cpp", 3, "PTonTransfer", (0x01F3835D,),
             "same branch checks m->opcode32() == kPTonTransfer"),
    HostCall("HostDedustDeposit.cpp", 1, "DedustDepositLiquidityToPool", (0xB56B9598,),
             "all helper inputs are the matcher's pool opcode anchor"),
    HostCall("HostDedustDeposit.cpp", 1, "JettonInternalTransfer", (0x178D4519,),
             "lp_transfer is assigned only by is_call_op(kJettonInternalTransfer)"),
    HostCall("HostDedustDeposit.cpp", 1, "JettonNotify", (0x7362D09C,),
             "resolve_leg obtains the jetton deposit through find_call(kJettonNotify)"),
    HostCall("HostDedustDeposit.cpp", 2, "JettonNotify", (0x7362D09C,),
             "the non-TON matcher leg is the exact deposit_notify contract branch"),
    HostCall("HostDedust.cpp", 1, "DedustSwapNotification", (0x9C610DE3,),
             "notif_blocks contains only is_call_op(kDedustSwapNotification)"),
    HostCall("HostDedust.cpp", 1, "DedustPayoutFromPool", (0xAD4EB6F5,),
             "payout_from_pool is filtered by the corrected wire opcode"),
    HostCall("HostCoffee.cpp", 1, "CoffeeSwapEvent", (0xC0FFEE30,),
             "event is returned by first_next_call(kCoffeeSwapSuccessfulEvent)"),
    HostCall("HostNft.cpp", 1, "NftOwnershipAssignedPrevOwner", (0x05138D91,),
             "the sole helper caller receives the exact assigned contract capture"),
    HostCall("HostJvault.cpp", 1, "JVaultStakePeriodPayload", (),
             "prefixless payload retains its arbitrary first 32-bit word as payload_opcode"),
)


def test_host_calls_remain_a_closed_opcode_dominated_manifest():
    call_re = re.compile(r'parse_message_body\(\s*"([^"]+)"')
    actual = set()
    for path in sorted(HOST.glob("*.cpp")):
        message_ordinals: dict[str, int] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            for message in call_re.findall(line):
                if message in ABI_PREFIXES:
                    ordinal = message_ordinals.get(message, 0) + 1
                    message_ordinals[message] = ordinal
                    actual.add((path.name, message, ordinal))

    reviewed = {(call.file, call.message, call.ordinal) for call in HOST_CALLS}
    assert actual == reviewed, (
        f"host parse call inventory drift (file, message, ordinal); "
        f"unreviewed={sorted(actual - reviewed)}, "
        f"missing={sorted(reviewed - actual)}"
    )

    for call in HOST_CALLS:
        expected = ABI_PREFIXES[call.message]
        if expected is None:
            assert call.guard_opcodes == (), call
        else:
            assert call.guard_opcodes == (expected,), call


def test_cocoon_parent_wallet_anchor_cannot_reach_build():
    artifact = emit_default()
    matcher = next(
        row for row in artifact["matchers"]
        if row["name"] == "cocoon_client_request_refund"
    )
    assert matcher["anchor"] == {
        "kind": "opcode_set", "values": [0x9C69F376, 0xFAFA6CC1],
    }

    root = artifact["nodes"][matcher["root"]]
    assert root["kind"] == "any" and root["capture"] == "refund"
    assert {
        artifact["nodes"][index]["opcode"] for index in root["children"]
    } == {0x2565934C, 0x65448FF4}
    parent = artifact["nodes"][root["parent"]]
    assert parent == {
        "kind": "contract",
        "opcode": 0x9C69F376,
        "optional": True,
        "capture": "wallet",
    }
