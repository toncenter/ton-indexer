"""Steady-state structural gate for ABI-faithful production message declarations."""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[3]
ABI_DIR = REPO / "ton-index-worker/ton-abi/abi"
# The ABI JSON is a build output of the submodule's tolk compiler; point
# TON_ABI_JSON_DIR at another build tree when it is not the default one.
ABI_JSON_DIR = Path(
    os.environ.get("TON_ABI_JSON_DIR", REPO / "build/ton-index-worker/ton-abi/abi")
)
ENGINE = REPO / "ton-index-worker/ton-mch-engine"
MSG_PARSE = ENGINE / "src/MsgParse.cpp"
ABI_BRIDGE = ENGINE / "src/AbiBridge.cpp"

# Logical public rows and their protocol-owned declaration files. The standard
# and Ethena internal-transfer structs intentionally share one logical key.
LOGICAL_ROWS_BY_STEM = {
    "jetton": (
        "JettonInternalTransfer", "JettonBurn", "JettonNotify", "JettonMint",
    ),
    "multisig": (
        "MultisigNewOrder", "MultisigInitOrder", "MultisigApprove",
        "MultisigApproveRejected", "MultisigExecute",
    ),
    "pton": ("PTonTransfer",),
    "stonfi": (
        "StonfiV2ProvideLiquidity", "StonfiPaymentRequest", "StonfiSwapMessage",
        "StonfiV2PayTo",
    ),
    "dedust": (
        "DedustPayoutFromPool", "DedustSwapNotification",
        "DedustDepositLiquidityToPool",
    ),
    "dedust_v2": (
        "DedustV2PayNative", "DedustV2SwapEvent", "DedustV2PayoutPositionFees",
        "DedustV2Withdraw", "DedustV2WithdrawalEvent", "DedustV2CreditAsset",
        "DedustV2JoinLiquidity", "DedustV2DepositEvent",
    ),
    "coffee": (
        "CoffeeCreateVault", "CoffeeSwapEvent",
        "CoffeeCreateLiquidityDepositoryRequest",
        "CoffeeDepositLiquiditySuccessfulEvent", "CoffeeLiquidityWithdrawalEvent",
        "CoffeeCreatePoolCreatorRequest", "CoffeeCreatePoolRequest",
        "CoffeeStakingClaimRewards", "CoffeeStakingPositionWithdraw2",
    ),
    "coffee_staking_withdraw3": ("CoffeeStakingPositionWithdraw3",),
    "evaa": (
        "EvaaSupplyMaster", "EvaaSupplySuccess", "EvaaWithdrawMaster",
        "EvaaWithdrawCollateralized",
    ),
    "evaa_supply_forward": ("EvaaSupplyJettonForward",),
    "jvault": ("JVaultUnstakeJettons", "JVaultUnstakeRequest", "JVaultClaim"),
    "jvault_payload": ("JVaultStakePeriodPayload",),
    "subscriptions": ("SubscriptionPaymentRequest",),
    "tonco": (
        "ToncoRouterV3SwapSourceWallet", "ToncoPoolV3StartBurn",
        "ToncoPositionNftV3PositionBurn", "ToncoPoolV3Burn",
        "ToncoRouterV3CreatePool", "ToncoPoolV3Init",
    ),
    "teleitem": ("TeleitemStartAuction",),
    "nft_sale": (
        "NftOwnershipAssignedPrevOwner", "SaleUpdateMessage", "NftReportStaticData",
    ),
    "tonstakers": ("TONStakersNftBurnNotification",),
    "cocoon": (
        "CocoonPayoutPayload", "CocoonLastPayoutPayload", "CocoonWorkerProxyRequest",
        "CocoonExtProxyPayoutRequest", "CocoonChargePayload",
        "CocoonGrantRefundPayload", "CocoonExtClientTopUp", "CocoonRegisterProxy",
        "CocoonUnregisterProxy", "CocoonOwnerClientRegister",
        "CocoonOwnerClientChangeSecretHash", "CocoonOwnerClientRequestRefund",
        "CocoonOwnerClientIncreaseStake", "CocoonOwnerClientWithdraw",
    ),
}

ETHENA_ARM = "JettonInternalTransferEthena"
EXTRA_STRUCTS = {"JettonInternalTransfer": (ETHENA_ARM,)}
ARM_NAMES = {
    "AssetTon", "AssetJetton",
    "JettonForwardPayloadRef", "JettonForwardPayloadInline",
    "PTonForwardPayloadRef", "PTonForwardPayloadInline",
}

STRUCT_RE = re.compile(
    r"\bstruct\s*(?:\((?P<prefix>0x[0-9a-fA-F]+|0b[01]+)\)\s*)?"
    r"(?P<name>[A-Za-z_]\w*)\s*\{(?P<body>.*?)\}",
    re.DOTALL,
)
FIELD_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*:\s*([^;\n]+)\s*;", re.MULTILINE)
ALIAS_RE = re.compile(r"^\s*type\s+([A-Za-z_]\w*)\s*=\s*([^;]+);", re.MULTILINE)


def _sources() -> dict[str, str]:
    return {
        stem: (ABI_DIR / f"{stem}.tolk").read_text(encoding="utf-8")
        for stem in LOGICAL_ROWS_BY_STEM
    }


def _structs(text: str) -> dict[str, tuple[str | None, str]]:
    found = {}
    for match in STRUCT_RE.finditer(text):
        name = match.group("name")
        assert name not in found, f"duplicate struct declaration {name}"
        found[name] = (match.group("prefix"), match.group("body"))
    return found


def _fields(body: str) -> list[tuple[str, str]]:
    return [(name, type_.strip()) for name, type_ in FIELD_RE.findall(body)]


def _allowed_arms(text: str) -> list[str]:
    match = re.search(
        r"\btype\s+AllowedMessage\s*=\s*(.*?)\n\s*\n\s*contract\b",
        text,
        re.DOTALL,
    )
    assert match, "missing AllowedMessage union"
    return re.findall(r"[A-Za-z_]\w*", match.group(1))


def test_a4_opcode_dispositions_are_frozen():
    sources = _sources()
    jetton = _structs(sources["jetton"])
    assert {int(jetton[name][0], 0) for name in ("JettonInternalTransfer", ETHENA_ARM)} == {
        0x178D4519, 0xB2583ED5,
    }
    assert int(jetton["JettonMint"][0], 0) == 0x642B7D07
    assert int(_structs(sources["coffee"])["CoffeeCreateVault"][0], 0) == 0xC0FFEE06
    assert "struct (0x00000000) CoffeeCreateVault" not in sources["coffee"]
    assert int(_structs(sources["dedust"])["DedustPayoutFromPool"][0], 0) == 0xAD4EB6F5
    assert "struct (0x474f86cf) DedustPayoutFromPool" not in sources["dedust"]
    assert int(_structs(sources["dedust_v2"])["DedustV2PayNative"][0], 0) == 0xA5A7CBF8
    assert int(_structs(sources["pton"])["PTonTransfer"][0], 0) == 0x01F3835D
    assert int(_structs(sources["cocoon"])["CocoonOwnerClientRequestRefund"][0], 0) == 0xFAFA6CC1
    assert "0x9c69f376" not in sources["cocoon"]

    prefix, body = _structs(sources["jvault_payload"])["JVaultStakePeriodPayload"]
    assert prefix is None
    assert _fields(body) == [("payload_opcode", "uint32"), ("stake_period", "uint32")]


def test_b4_union_arm_names_and_shapes_are_exact():
    sources = _sources()
    all_text = "\n".join(sources.values())
    family_spellings = set(re.findall(
        r"\b(?:Asset(?:Ton|Jetton)\w*|(?:Jetton|PTon)ForwardPayload\w*)\b",
        all_text,
    ))
    assert family_spellings == ARM_NAMES

    for stem, ton_prefix, jetton_prefix in (
        ("coffee", "0b00", "0b01"), ("dedust", "0b0000", "0b0001"),
    ):
        structs = _structs(sources[stem])
        assert structs["AssetTon"][0] == ton_prefix
        assert _fields(structs["AssetTon"][1]) == []
        assert structs["AssetJetton"][0] == jetton_prefix
        assert _fields(structs["AssetJetton"][1]) == [
            ("workchain", "uint8"), ("hash", "bits256"),
        ]

    for stem, alias, ref_arm, inline_arm in (
        ("jetton", "JettonForwardTail", "JettonForwardPayloadRef", "JettonForwardPayloadInline"),
        ("pton", "PTonForwardTail", "PTonForwardPayloadRef", "PTonForwardPayloadInline"),
    ):
        structs = _structs(sources[stem])
        assert _fields(structs[ref_arm][1]) == [("value", "cell")]
        assert _fields(structs[inline_arm][1]) == [("value", "RemainingBitsAndRefs")]
        assert re.search(rf"type\s+{alias}\s*=\s*{ref_arm}\s*\|\s*{inline_arm}", sources[stem])


def test_message_opcodes_are_unique_within_each_protocol_file():
    sources = _sources()
    for stem, logical_names in LOGICAL_ROWS_BY_STEM.items():
        structs = _structs(sources[stem])
        concrete = list(logical_names)
        for logical_name in logical_names:
            concrete.extend(EXTRA_STRUCTS.get(logical_name, ()))
        prefixes = [structs[name][0] for name in concrete if structs[name][0] is not None]
        normalized = [int(prefix, 0) for prefix in prefixes]
        duplicates = {opcode for opcode, count in Counter(normalized).items() if count > 1}
        assert not duplicates, f"{stem}: duplicate opcodes {sorted(duplicates)}"

    assert _structs(sources["evaa"])["EvaaSupplyMaster"][0] == "0x00000001"
    assert _structs(sources["evaa_supply_forward"])["EvaaSupplyJettonForward"][0] == "0x00000001"
    assert _structs(sources["coffee"])["CoffeeStakingPositionWithdraw2"][0] == "0xcb03bfaf"
    assert _structs(sources["coffee_staking_withdraw3"])["CoffeeStakingPositionWithdraw3"][0] == "0xcb03bfaf"


def test_generated_abi_incoming_rows_match_protocol_sources():
    sources = _sources()

    def body_struct_name(abi, body_ty_idx):
        body_type = abi["unique_types"][body_ty_idx]
        if body_type["kind"] == "StructRef":
            return body_type["struct_name"]
        alias = next(
            declaration for declaration in abi["declarations"]
            if declaration["kind"] == "alias" and declaration["name"] == body_type["alias_name"]
        )
        struct = next(
            declaration for declaration in abi["declarations"]
            if declaration["kind"] == "struct" and declaration["ty_idx"] == alias["target_ty_idx"]
        )
        return struct["name"]

    if not ABI_JSON_DIR.is_dir():
        pytest.skip("no build-tree ABI JSON (build ton-abi or set TON_ABI_JSON_DIR)")
    for stem, text in sources.items():
        abi = json.loads((ABI_JSON_DIR / f"{stem}.abi.json").read_text(encoding="utf-8"))
        incoming_names = {
            body_struct_name(abi, row["body_ty_idx"])
            for row in abi["incoming_messages"]
        }
        assert incoming_names == set(_allowed_arms(text))


def test_obsolete_message_schema_pipeline_is_absent_from_live_source():
    deleted = (
        ENGINE / ("mch-msgs" + ".schema"),
        ENGINE / ("src/MsgParsers" + "Generated.cpp"),
        ENGINE / ("src/MsgParsers" + "Generated.h"),
        ENGINE / ("src/MsgVector" + "Runner.cpp"),
        ENGINE / ("src/MsgVector" + "Runner.h"),
        ENGINE / ("ir/msg_" + "vectors.json"),
        REPO / ("indexer/indexer/events/mch/msgs" + "_gen.py"),
        REPO / ("indexer/tests/mch/test_msgs" + "_gen.py"),
    )
    assert not any(path.exists() for path in deleted)

    forbidden = (
        "mch-msgs" + ".schema",
        "msgs" + "_gen",
        "MsgParsers" + "Generated",
        "generated_message" + "_parsers",
        "schema-generated" + " parser",
        "mch_" + "legacy",
        "schema_to_faithful" + "_tolk",
    )
    roots = (REPO / "indexer/indexer/events/mch", REPO / "indexer/tests/mch", ENGINE / "src")
    offenders = []
    for root in roots:
        for path in root.rglob("*"):
            if path == Path(__file__) or path.suffix not in {".py", ".mch", ".cpp", ".h"}:
                continue
            text = path.read_text(encoding="utf-8")
            for term in forbidden:
                if term in text:
                    offenders.append(f"{path.relative_to(REPO)}: {term}")
    cmake = (ENGINE / "CMakeLists.txt").read_text(encoding="utf-8")
    offenders.extend(f"ton-index-worker/ton-mch-engine/CMakeLists.txt: {term}" for term in forbidden if term in cmake)
    assert not offenders, "obsolete pipeline references:\n" + "\n".join(offenders)
