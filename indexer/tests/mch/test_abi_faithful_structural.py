"""Steady-state structural gate for ABI-faithful production message declarations."""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from pathlib import Path


REPO = Path(__file__).resolve().parents[3]
ABI_DIR = REPO / "ton-index-worker/ton-abi/abi"
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

EXISTING_ABI_ROWS = {"ClaimRewardMessage", "PayoutRewardMessage", "PayoutMessage"}
ETHENA_ARM = "JettonInternalTransferEthena"
EXTRA_STRUCTS = {"JettonInternalTransfer": (ETHENA_ARM,)}
ARM_NAMES = {
    "AssetTon", "AssetJetton",
    "JettonForwardPayloadRef", "JettonForwardPayloadInline",
    "PTonForwardPayloadRef", "PTonForwardPayloadInline",
}

# Hashes cover the normalized declaration surface only: struct order, prefixes,
# visible field order/types, and aliases. Compiler scaffolding and comments do
# not affect them. This is the steady-state replacement for the deleted source
# census and freezes all 70 declarations without another schema frontend.
DECL_FINGERPRINTS = {
    "jetton": "28ff2075dadedd370f9ad0b66cf4a1ae4999489e0c8ee13e34e0d9316e2df856",
    "multisig": "66138be810df1efc2cc6832edba8a3808dafa6a2068bb246b129d0d93fbb96b6",
    "pton": "32f1e8f3a8dc61450822176912481c87cbd21a2cff05d94140079d94b9f266a3",
    "stonfi": "b33f69ff2bc8b92508153ec42542b577540dea0535329d7ab5c54e8c027af607",
    "dedust": "93cd27433ad48a6a3839c77a59284562c3a47cb78a4ba7693d59c4d99d62ab37",
    "dedust_v2": "b0cd7a5a1ad7a9d636e56b745e28a41623137f826d44def67ea9330021dc4ffb",
    "coffee": "50340813be1e520d951deeda9c17af65f50934a178d62cf92ac9c71b187d2542",
    "coffee_staking_withdraw3": "ddee217b44abed1ee074535621d34174ed34438024d4a0d473e33f74fc2dbefc",
    "evaa": "39b1fab15eeb563d55d32241a01c0ec523fe50daff37d2efceaf22cb895e6c23",
    "evaa_supply_forward": "8730c59decf959ebbb08321b0144d2e396498da056d5bf142ab4bfb757c2227f",
    "jvault": "b831525189108dd4c09c463269463ff5826774131127e134c4f2541106fd162d",
    "jvault_payload": "fedef8b1621ecd95964ef8572d1db437d1e331dd7ab3b9b113fe460da0cbaa36",
    "subscriptions": "d955927476b60b04a00c34755d508008a9814829151cd64d488a283bc153d3dd",
    "tonco": "01d0def6dcf3b8f602e08331cd41fbb2e2ae53519402da50d6ff4454df7ae6a4",
    "teleitem": "520fae90a066bd9d90515cb2c9698e90784bfcd38a5e0f1c53f6a983cf3556b8",
    "nft_sale": "402832c07361415882afc081be3a7ef5eca86a6574c6069b23660eff0e3edfb8",
    "tonstakers": "c0b64a38368c9f24078d35200378f5c113ead2e7904b8030f54e5fc42cfe3cbb",
    "cocoon": "72ef404ac850ac393bba6dec1feb3ecab330ed2cd05875dbc92e94afae2f87f0",
}

EXPECTED_CELL_FIELDS = {
    "StonfiPaymentRequest.info": "Cell<StonfiPaymentRequestInfo>",
    "StonfiSwapMessage.info": "Cell<StonfiSwapInfo>",
    "StonfiV2PayTo.info": "Cell<StonfiV2PayToInfo>",
    "DedustSwapNotification.info": "Cell<DedustSwapInfo>",
    "DedustDepositLiquidityToPool.field4": "Cell<DedustDepositLiquidityAssets>",
    "CoffeeCreateLiquidityDepositoryRequest._params": "Cell<CoffeeDepositParams>",
    "CoffeeCreateLiquidityDepositoryRequest.pool_params": "Cell<CoffeePoolParams>",
    "CoffeeCreatePoolCreatorRequest.creation_params": "Cell<CoffeePoolCreationParams>",
    "ToncoPositionNftV3PositionBurn._old_fee": "Cell<ToncoPositionOldFee>",
    "ToncoPoolV3Burn._old_fee": "Cell<ToncoPoolOldFee>",
    "ToncoPoolV3Burn._new_fee": "Cell<ToncoPoolNewFee>",
    "ToncoRouterV3CreatePool.minters": "Cell<ToncoPoolMinters>",
    "TeleitemStartAuction.config": "Cell<TeleitemAuctionConfig>",
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


def _logical_rows() -> set[str]:
    rows = {name for names in LOGICAL_ROWS_BY_STEM.values() for name in names}
    assert len(rows) == 70
    return rows


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
        r"\btype\s+AllowedMessage\s*=\s*(.*?)\n\s*\n\s*struct\s+Storage\b",
        text,
        re.DOTALL,
    )
    assert match, "missing AllowedMessage union"
    return re.findall(r"[A-Za-z_]\w*", match.group(1))


def _registry_names(path: Path, function: str) -> list[str]:
    text = path.read_text(encoding="utf-8")
    match = re.search(
        rf"\b{re.escape(function)}\(\).*?\brows\s*=\s*\{{(.*?)\n\s*\}};",
        text,
        re.DOTALL,
    )
    assert match, f"could not read {function} rows from {path.name}"
    return re.findall(r'\{\s*"([A-Za-z_]\w*)"\s*,', match.group(1))


def _fingerprint(text: str) -> str:
    payload = {
        "structs": [
            (m.group("name"), m.group("prefix"), _fields(m.group("body")))
            for m in STRUCT_RE.finditer(text)
        ],
        "aliases": [(name, " ".join(value.split())) for name, value in ALIAS_RE.findall(text)],
    }
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_exactly_70_logical_rows_are_owned_and_cut_over():
    sources = _sources()
    migrated = _logical_rows()
    hand = _registry_names(MSG_PARSE, "hand_message_parsers")
    abi = _registry_names(ABI_BRIDGE, "abi_message_parsers")

    assert len(hand) == len(set(hand)) == 12
    assert len(abi) == len(set(abi)) == 73
    assert set(abi) == migrated | EXISTING_ABI_ROWS
    assert migrated.isdisjoint(hand)

    incoming_rows = [name for text in sources.values() for name in _allowed_arms(text)]
    assert Counter(incoming_rows) == Counter(migrated | {ETHENA_ARM} | EXISTING_ABI_ROWS)
    for stem, names in LOGICAL_ROWS_BY_STEM.items():
        structs = _structs(sources[stem])
        for name in names:
            assert name in structs
            assert name in _allowed_arms(sources[stem])

def test_frozen_declaration_surface_and_cell_topology():
    sources = _sources()
    assert {stem: _fingerprint(text) for stem, text in sources.items()} == DECL_FINGERPRINTS

    cells = {}
    for text in sources.values():
        for struct_name, (_, body) in _structs(text).items():
            for field_name, type_ in _fields(body):
                if type_.startswith("Cell<"):
                    cells[f"{struct_name}.{field_name}"] = type_
    assert cells == EXPECTED_CELL_FIELDS

    assert "signers_hash: bits256;" in sources["multisig"]
    assert "type JVaultClaimEntry = RemainingBitsAndRefs" in sources["jvault"]
    assert "jettons_to_claim: map<address, JVaultClaimEntry>;" in sources["jvault"]
    jetton_structs = _structs(sources["jetton"])
    assert _fields(jetton_structs[ETHENA_ARM][1]) == _fields(
        jetton_structs["JettonInternalTransfer"][1]
    )


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

    for stem, text in sources.items():
        abi = json.loads((ABI_DIR / f"{stem}.abi.json").read_text(encoding="utf-8"))
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
